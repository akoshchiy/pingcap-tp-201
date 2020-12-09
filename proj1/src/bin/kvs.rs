use std::env;
use clap::{Arg, App, load_yaml};
use std::fs::read_to_string;
use std::process::exit;


fn main() {
    let yaml = load_yaml!("cli.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
    }

    match matches.subcommand() {
        Some(("get", args)) => { error_exit("unimplemented"); }
        Some(("set", args)) => { error_exit("unimplemented"); }
        Some(("rm", args)) => { error_exit("unimplemented"); }
        Some((cmd, args)) => { error_exit(format!("{}{}", "unknown command: ", cmd).as_str()); }
        _ => { unreachable!(); }
    }
}

fn error_exit(msg: &str) {
    eprintln!("{}", msg);
    exit(1);
}
