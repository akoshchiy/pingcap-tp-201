use crate::kvs::{Result, KvError};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use serde::de::DeserializeOwned;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "cmd")]
pub(crate) enum Command {
    Get { key: String },
    Set { key: String, val: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "t")]
pub(crate) enum CommandResult {
    Ok,
    OkVal(String),
    Err(String),
}

pub(crate) fn read<R: Read, V: DeserializeOwned>(reader: &mut R) -> Result<V> {
    let size = reader.read_u32::<BigEndian>()?;
    let mut buf = vec![0; size as usize];
    reader.read_exact(&mut buf)?;
    bson::from_slice(&buf)
        .map_err(|e| KvError::BsonDeserialize(e))
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
