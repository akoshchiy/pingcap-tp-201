use clap::{load_yaml, App};
use proj2::kvs::KvError;
use proj2::kvs::KvStore;

use std::env;
use std::process::exit;

use proj2::kvs;
use std::borrow::Borrow;
use std::error::Error;
use std::fs::File;

fn main() {
    let dir = env::current_dir().unwrap();
    let mut store = KvStore::open(dir).unwrap();

    let yaml = load_yaml!("cli.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
    }

    match matches.subcommand() {
        Some(("get", args)) => {
            let key = args.value_of("key").unwrap();
            let result = store
                .get(key.to_string())
                .unwrap()
                .unwrap_or("Key not found".to_string());
            println!("{}", result);
        }
        Some(("set", args)) => {
            let key = args.value_of("key").unwrap();
            let value = args.value_of("value").unwrap();
            store.set(key.to_string(), value.to_string()).unwrap();
        }
        Some(("rm", args)) => {
            let key = args.value_of("key").unwrap();
            match store.remove(key.to_string()) {
                Ok(_) => return,
                Err(err) => {
                    match err {
                        KvError::KeyNotFound => println!("Key not found"),
                        _ => println!("{:?}", err),
                    }
                    exit(1);
                }
            };
        }
        _ => {
            unreachable!();
        }
    }
}
