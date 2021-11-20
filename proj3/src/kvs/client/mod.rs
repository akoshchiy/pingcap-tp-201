use slog::{Logger, trace, o};
use std::net::{SocketAddr, TcpStream};

use crate::kvs::{Result, KvError};
use crate::kvs::net::{write, read, Command, CommandResult};
use serde::de::DeserializeOwned;

pub struct KvsClient {
    log: Logger,
    stream: TcpStream,
}

impl KvsClient {
    pub fn connect(log: &Logger, addr: SocketAddr) -> Result<KvsClient> {
        let stream = TcpStream::connect(addr)?;
        Ok(
            KvsClient {
                log: log.new(o!()),
                stream,
            }
        )
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.write_cmd(Command::Get { key });

        let result = self.read_result()?;

        match result {
            CommandResult::Ok => Ok(Option::None),
            CommandResult::OkVal(val) => Ok(Option::Some(val)),
            CommandResult::Err(err) => Err(KvError::Server { msg: err })
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.write_cmd(Command::Remove { key })?;
        let result = self.read_result()?;
        parse_void_response(result)
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.write_cmd(Command::Set { key, val })?;
        let result = self.read_result()?;
        parse_void_response(result)
    }

    fn write_cmd(&mut self, cmd: Command) -> Result<()> {
        trace!(self.log, "command: {}", &cmd);
        write(&mut self.stream, &cmd)
    }

    fn read_result(&mut self) -> Result<CommandResult> {
        let result: CommandResult = read(&mut self.stream)?;
        trace!(self.log, "response: {}", &result);
        Ok(result)
    }
}

fn parse_void_response(result: CommandResult) -> Result<()> {
    match result {
        CommandResult::Ok => Ok(()),
        CommandResult::Err(err) => Err(KvError::Server { msg: err }),
        CommandResult::OkVal(val) => Err(KvError::UnexpectedResult { val })
    }
}