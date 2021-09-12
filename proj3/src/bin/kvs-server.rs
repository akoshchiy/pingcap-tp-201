use clap::{load_yaml, App, ArgMatches};
use proj3::kvs::{KvError, KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};
use sled::Db;
use slog::{info, o, Drain, Logger};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::{env, format};

fn main() {
    let log = init_log();

    let yaml = load_yaml!("kvs-client.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    let dir = env::current_dir().unwrap();

    let addr = parse_addr(&log, &matches);
    let engine_name = parse_engine(&log, &matches);

    start_server(&engine_name, dir.as_path(), addr).expect("server start failed");
}

fn parse_addr(log: &Logger, matches: &ArgMatches) -> SocketAddr {
    let addr_str = matches.value_of("addr").unwrap_or("127.0.0.1:4000");

    info!(log, "addr: {}", addr_str);

    addr_str.parse().expect("parse addr failed")
}

fn parse_engine(log: &Logger, matches: &ArgMatches) -> String {
    let engine = matches.value_of("engine").unwrap_or("kvs");
    info!(log, "engine: {}", engine);
    engine.to_string()
}

fn start_server(engine: &str, root_path: &Path, addr: SocketAddr) -> Result<()> {
    match engine {
        "kvs" => build_kvs(root_path).and_then(|e| KvsServer::new(e).listen(addr)),
        "sled" => build_sled(root_path).and_then(|e| KvsServer::new(e).listen(addr)),
        _ => panic!("undefined engine: {}", engine),
    }
}

fn build_sled(file_path: &Path) -> Result<SledKvsEngine> {
    sled::open(file_path)
        // TODO new err type for sled init
        .map_err(|err| KvError::Sled {
            key: String::new(),
            source: err,
        })
        .map(|db| SledKvsEngine::new(db))
}

fn build_kvs(file_path: &Path) -> Result<KvStore> {
    KvStore::open(file_path)
}

fn init_log() -> Logger {
    let decorator = slog_term::PlainDecorator::new(std::io::stderr());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
