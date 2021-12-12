use crate::kvs::net::{read, write, Command, CommandResult};
use crate::kvs::KvsEngine;
use crate::kvs::Result;
use slog::{info, Logger};
use std::net::TcpStream;

pub struct ConnectionHandler<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> ConnectionHandler<E> {
    pub fn new(engine: E, log: Logger) -> ConnectionHandler<E> {
        ConnectionHandler { engine, log }
    }

    pub fn handle(&self, mut stream: TcpStream) -> Result<()> {
        let addr = stream.peer_addr()?;
        info!(self.log, "connection from: {}", addr.ip());

        let cmd: Command = read(&mut stream)?;
        match cmd {
            Command::Set { key, val } => self.handle_set(key, val, &mut stream),
            Command::Get { key } => self.handle_get(key, &mut stream),
            Command::Remove { key } => self.handle_remove(key, &mut stream),
        }
    }

    fn handle_set(&self, key: String, val: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.set(key, val);
        match result {
            Ok(_) => write(stream, &CommandResult::Ok),
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }

    fn handle_get(&self, key: String, stream: &mut TcpStream) -> Result<()> {
        let res = self.engine.get(key);
        match res {
            Ok(val) => match val {
                Some(v) => write(stream, &CommandResult::OkVal(v)),
                None => write(stream, &CommandResult::Ok),
            },
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }

    fn handle_remove(&self, key: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.remove(key);
        match result {
            Ok(_) => write(stream, &CommandResult::Ok),
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }
}
