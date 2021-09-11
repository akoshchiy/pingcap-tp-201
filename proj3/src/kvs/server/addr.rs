use std::net::{IpAddr, AddrParseError};
use std::str::FromStr;
use thiserror::Error;
use std::num::ParseIntError;

#[derive(Error, Debug)]
pub enum AddrError {
    #[error("format error: {val}")]
    Format { val: String },

    #[error("addr parse error: {val}, source -> {source}")]
    AddrParse { val: String, source: AddrParseError },

    #[error("port parse error: {val}, source -> {source}")]
    PortParse { val: String, source: ParseIntError },
}

pub struct ServerAddr(IpAddr, u32);

impl FromStr for ServerAddr {
    type Err = AddrError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let split: Vec<_> = s.split(":").collect();

        if split.len() != 2 {
            return Err(AddrError::Format { val: s.to_string() });
        }

        let addr: IpAddr = split[0].parse()
            .map_err(|e| AddrError::AddrParse { val: split[0].to_string(), source: e })?;

        let port: u32 = split[1].parse()
            .map_err(|e| AddrError::PortParse { val: split[1].to_string(), source: e })?;

        Ok(ServerAddr(addr, port))
    }
}
