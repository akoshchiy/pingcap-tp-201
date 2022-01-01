use clap::{load_yaml, App, AppSettings, ArgMatches};

use proj5::kvs::{KvError, KvStore, KvsClient};
use slog::{info, o, Drain, Logger};
use std::borrow::Borrow;
use std::env;
use std::error::Error;
use std::fs::File;
use std::net::SocketAddr;
use std::process::exit;
use tokio::runtime::Builder;

fn main() {
    let log = init_log();
    let yaml = load_yaml!("kvs-client.yml");

    let app = App::from(yaml).setting(AppSettings::ArgRequiredElseHelp);

    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    match matches.subcommand() {
        Some(("get", args)) => {
            let addr = parse_addr(&log, &args);
            let key = args.value_of("key").unwrap();

            runtime.block_on(async move {
                let result = KvsClient::connect(&log, addr)
                    .await
                    .unwrap()
                    .get(key.to_string())
                    .await
                    .unwrap()
                    .unwrap_or("Key not found".to_string());
                println!("{}", result);
            });
        }
        Some(("set", args)) => {
            let addr = parse_addr(&log, &args);
            let key = args.value_of("key").unwrap();
            let value = args.value_of("value").unwrap();

            runtime.block_on(async move {
                KvsClient::connect(&log, addr)
                    .await
                    .unwrap()
                    .set(key.to_string(), value.to_string())
                    .await
                    .unwrap();
            });
        }
        Some(("rm", args)) => {
            let addr = parse_addr(&log, &args);
            let key = args.value_of("key").unwrap();

            runtime.block_on(async move {
                let mut client = KvsClient::connect(&log, addr).await.unwrap();
                match client.remove(key.to_string()).await {
                    Ok(_) => exit(0),
                    Err(err) => {
                        eprintln!("{:?}", err);
                        exit(1);
                    }
                };
            });
        }
        _ => {
            unreachable!();
        }
    }
}

fn parse_addr(log: &Logger, matches: &ArgMatches) -> SocketAddr {
    let addr_str = matches.value_of("addr").unwrap_or("127.0.0.1:4000");

    info!(log, "addr: {}", addr_str);

    addr_str.parse().expect("parse addr failed")
}

fn init_log() -> Logger {
    let decorator = slog_term::PlainDecorator::new(std::io::stderr());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
