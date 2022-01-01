use slog::{o, trace, Logger};
use std::net::{SocketAddr};

use crate::kvs::net::{read, write, Command, CommandResult, write_async, read_async};
use crate::kvs::{KvError, Result};
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;

pub struct KvsClient {
    log: Logger,
    stream: TcpStream,
}

impl KvsClient {
    pub async fn connect(log: &Logger, addr: SocketAddr) -> Result<KvsClient> {
        let stream = TcpStream::connect(addr).await?;
        Ok(KvsClient {
            log: log.new(o!()),
            stream,
        })
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        self.write_cmd(Command::Get { key }).await?;

        let result = self.read_result().await?;

        match result {
            CommandResult::Ok => Ok(Option::None),
            CommandResult::OkVal(val) => Ok(Option::Some(val)),
            CommandResult::Err(err) => Err(KvError::Server { msg: err }),
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        self.write_cmd(Command::Remove { key }).await?;
        let result = self.read_result().await?;
        parse_void_response(result)
    }

    pub async fn set(&mut self, key: String, val: String) -> Result<()> {
        self.write_cmd(Command::Set { key, val }).await?;
        let result = self.read_result().await?;
        parse_void_response(result)
    }

    async fn write_cmd(&mut self, cmd: Command) -> Result<()> {
        trace!(self.log, "command: {}", &cmd);
        write_async(&mut self.stream, &cmd)
    }

    async fn read_result(&mut self) -> Result<CommandResult> {
        let result: CommandResult = read_async(&mut self.stream).await?;
        trace!(self.log, "response: {}", &result);
        Ok(result)
    }
}

fn parse_void_response(result: CommandResult) -> Result<()> {
    match result {
        CommandResult::Ok => Ok(()),
        CommandResult::Err(err) => Err(KvError::Server { msg: err }),
        CommandResult::OkVal(val) => Err(KvError::UnexpectedResult { val }),
    }
}
