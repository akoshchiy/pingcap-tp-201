use clap::{load_yaml, App};
use proj2::kvs::KvStore;
use std::env;
use std::process::exit;

use std::error::Error;
use std::fs::File;
use thiserror::Error;
// use std::backtrace::Backtrace;

#[derive(Error, Debug)]
enum MyError {
    #[error(transparent)]
    Test { #[from] source: Box<dyn Error> },
}

fn main() {
    // let store = KvStore::open("sdad").unwrap();

    match test_err() {
        Ok(_) => return,
        Err(e) => {
            eprintln!("error: {}", e);
            exit(1);
        }
    }

    // let yaml = load_yaml!("cli.yml");
    // let app = App::from(yaml);
    // let matches = app.get_matches();
    //
    // if matches.is_present("version") {
    //     println!(env!("CARGO_PKG_VERSION"));
    // }
    //
    // match matches.subcommand() {
    //     Some(("get", _args)) => {
    //         error_exit("unimplemented");
    //     }
    //     Some(("set", _args)) => {
    //         error_exit("unimplemented");
    //     }
    //     Some(("rm", _args)) => {
    //         error_exit("unimplemented");
    //     }
    //     _ => {
    //         unreachable!();
    //     }
    // }
}

fn test_err() -> Result<(), MyError> {
    match File::open("foo.txt") {
        Ok(_) => Ok(()),
        Err(e) => Err(MyError::Test {
            source: Box::new(e)
            // msg: "fucked".to_string(),
            // source: Box::new(e),
            // backtrace: Backtrace::capture(),
        }),
    }
}

fn error_exit(msg: &str) {
    eprintln!("{}", msg);
    exit(1);
}
