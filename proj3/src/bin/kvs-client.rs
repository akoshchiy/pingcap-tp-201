use clap::{load_yaml, App};

use proj3::kvs::{KvError, KvStore};
use std::borrow::Borrow;
use std::env;
use std::error::Error;
use std::fs::File;
use std::process::exit;

fn main() {
    let yaml = load_yaml!("kvs-client.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
    }

    match matches.subcommand() {
        Some(("get", args)) => {
            // let key = args.value_of("key").unwrap();
            // let result = store
            //     .get(key.to_string())
            //     .unwrap()
            //     .unwrap_or("Key not found".to_string());
            // println!("{}", result);
            unimplemented!()
        }
        Some(("set", args)) => {
            // let key = args.value_of("key").unwrap();
            // let value = args.value_of("value").unwrap();
            // store.set(key.to_string(), value.to_string()).unwrap();
            unimplemented!()
        }
        Some(("rm", args)) => {
            // let key = args.value_of("key").unwrap();
            // match store.remove(key.to_string()) {
            //     Ok(_) => return,
            //     Err(err) => {
            //         match err {
            //             KvError::KeyNotFound => println!("Key not found"),
            //             _ => println!("{:?}", err),
            //         }
            //         exit(1);
            //     }
            // };
            unimplemented!()
        }
        _ => {
            unreachable!();
        }
    }
}
