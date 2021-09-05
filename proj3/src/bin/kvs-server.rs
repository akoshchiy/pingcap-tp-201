use clap::{load_yaml, App};
use std::net::IpAddr;
use std::str::FromStr;
use proj3::kvs::{KvsServer, KvStore, SledKvsEngine, KvsEngine, Result, KvError};
use std::path::Path;
use sled::Db;
use std::env;

fn main() {
    let yaml = load_yaml!("kvs-client.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    let dir = env::current_dir().unwrap();

    let addr = parse_addr(matches.value_of("addr").unwrap_or("localhost:4000"));

    let engine_name = matches.value_of("engine").unwrap_or("kvs");

    start_server(engine_name, dir.as_path(), addr)
        .expect("server start failed");
}

fn parse_addr(addr: &str) -> IpAddr {
    IpAddr::from_str(addr).expect("bad addr format")
}

fn start_server(engine: &str, root_path: &Path, addr: IpAddr) -> Result<()> {
    match engine {
        "kvs" => build_kvs(root_path)
            .and_then(|e| KvsServer::new(e).listen(addr)),
        "sled" => build_sled(root_path).and_then(|e| KvsServer::new(e).listen(addr)),
        _ => panic!("undefined engine: {}", engine)
    }
}

fn build_sled(file_path: &Path) -> Result<impl KvsEngine> {
    sled::open(file_path)
        // TODO new err type for sled init
        .map_err(|err| KvError::Sled { key: String::new(), source: err })
        .map(|db| SledKvsEngine::new(db))
}

fn build_kvs(file_path: &Path) -> Result<impl KvsEngine> {
    KvStore::open(file_path)
}