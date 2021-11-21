use crate::kvs::net::Command;
use crate::kvs::net::{read, write, CommandResult};
use crate::kvs::{KvsEngine, Result};
use slog::{error, info, trace, Logger};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ptr::write_bytes;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E, log: Logger) -> KvsServer<E> {
        KvsServer { engine, log }
    }

    pub fn listen(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            let res = self.handle_conn(stream?);
            match res {
                Ok(()) => continue,
                Err(e) => {
                    let err_str = e.to_string();
                    error!(&self.log, "error from handle_conn: {}", err_str);
                }
            }
        }

        Ok(())
    }

    fn handle_conn(&mut self, mut stream: TcpStream) -> Result<()> {
        let addr = stream.peer_addr()?;
        info!(self.log, "connection from: {}", addr.ip());

        let cmd: Command = read(&mut stream)?;
        match cmd {
            Command::Set { key, val } => self.handle_set(key, val, &mut stream),
            Command::Get { key } => self.handle_get(key, &mut stream),
            Command::Remove { key } => self.handle_remove(key, &mut stream),
        }
    }

    fn handle_set(&mut self, key: String, val: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.set(key, val);
        match result {
            Ok(_) => write(stream, &CommandResult::Ok),
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }

    fn handle_get(&mut self, key: String, stream: &mut TcpStream) -> Result<()> {
        let res = self.engine.get(key);
        match res {
            Ok(val) => match val {
                Some(v) => write(stream, &CommandResult::OkVal(v)),
                None => write(stream, &CommandResult::Ok),
            },
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }

    fn handle_remove(&mut self, key: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.remove(key);
        match result {
            Ok(_) => write(stream, &CommandResult::Ok),
            Err(e) => write(stream, &CommandResult::Err(e.to_string())),
        }
    }
}
