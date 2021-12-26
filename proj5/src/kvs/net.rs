use crate::kvs::{KvError, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "cmd")]
pub(crate) enum Command {
    Get { key: String },
    Set { key: String, val: String },
    Remove { key: String },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Get { key } => write!(f, "Get({})", key),
            Command::Set { key, val } => write!(f, "Set({}, {})", key, val),
            Command::Remove { key } => write!(f, "Remove({})", key),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "t", content = "__field0")]
pub(crate) enum CommandResult {
    Ok,
    OkVal(String),
    Err(String),
}

impl Display for CommandResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandResult::Ok => write!(f, "Ok"),
            CommandResult::OkVal(val) => write!(f, "OkVal({})", val),
            CommandResult::Err(err) => write!(f, "Err({})", err),
        }
    }
}

pub(crate) fn read<R: Read, V: DeserializeOwned>(reader: &mut R) -> Result<V> {
    let size = reader.read_u32::<BigEndian>()?;
    let mut buf = vec![0; size as usize];
    reader.read_exact(&mut buf)?;
    bson::from_slice(&buf).map_err(|e| KvError::BsonDeserialize(e))
}

pub(crate) fn write<W: Write, V: Serialize>(writer: &mut W, val: &V) -> Result<()> {
    let buf = bson::to_vec(val)?;
    let size = buf.len();
    writer.write_u32::<BigEndian>(size as u32);
    writer.write(&buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::kvs::net::{read, write, Command};
    use std::io::Cursor;

    #[test]
    fn test_read_write() {
        let cmd = Command::Set {
            key: "key".to_string(),
            val: "val".to_string(),
        };

        let mut buf = Vec::new();

        write(&mut buf, &cmd).unwrap();
        let read_cmd: Command = read(&mut Cursor::new(&buf)).unwrap();

        assert_eq!(read_cmd, cmd);
    }
}
