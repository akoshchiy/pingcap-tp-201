use clap::{load_yaml, App, ArgMatches};
use proj5::kvs::thread_pool::{
    NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool,
};
use proj5::kvs::{KvError, KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};
use sled::Db;
use slog::{info, o, Drain, Logger};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::{env, format};
use tokio::runtime::{Builder, Runtime};

fn main() {
    let log = init_log();

    let yaml = load_yaml!("kvs-server.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    info!(log, "version: {}", env!("CARGO_PKG_VERSION"));

    let dir = env::current_dir().unwrap();
    let addr = parse_addr(&log, &matches);
    let engine_name = parse_engine(&log, &matches);


    start_server(&log, &engine_name, dir.as_path(), addr)
        .expect("failed to start server");
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

fn start_server(root_log: &Logger, engine: &str, root_path: &Path, addr: SocketAddr) -> Result<()> {
    let log = root_log.new(o!());
    match engine {
        "kvs" => {
            let kvs = build_kvs(root_log, root_path)?;
            start_with(kvs, addr, log);
        }
        "sled" => {
            let sled = build_sled(root_path)?;
            start_with(sled, addr, log);

        },
        _ => panic!("undefined engine: {}", engine),
    };
    Ok(())
}

fn start_with<E: KvsEngine>(engine: E, addr: SocketAddr, log: Logger) {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    let server = KvsServer::new(engine, log);

    runtime.block_on(async move {
        server.listen(addr).await;
    });
}

fn build_sled(file_path: &Path) -> Result<SledKvsEngine<RayonThreadPool>> {
    if check_engine_data(file_path, "kvs") {
        panic!("kvs engine data dir detected");
    }

    let sled_path = file_path.join("sled_data");

    std::fs::create_dir_all(sled_path.as_path())?;
    sled::open(sled_path.as_path())
        .map_err(|err| KvError::Sled(err))
        .and_then(|db| SledKvsEngine::new(db, num_cpus::get() as u32))
}

fn build_kvs(log: &Logger, file_path: &Path) -> Result<KvStore<RayonThreadPool>> {
    if check_engine_data(file_path, "sled") {
        panic!("sled engine data dir detected");
    }

    let kvs_path = file_path.join("kvs_data");

    std::fs::create_dir_all(kvs_path.as_path())?;
    info!(log, "kvs path: {}", kvs_path.display().to_string());
    KvStore::open(kvs_path.as_path(), num_cpus::get() as u32)
}

fn init_log() -> Logger {
    let decorator = slog_term::PlainDecorator::new(std::io::stderr());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

fn check_engine_data(file_path: &Path, engine: &str) -> bool {
    file_path
        .join(engine.to_owned() + "_data")
        .as_path()
        .exists()
}
