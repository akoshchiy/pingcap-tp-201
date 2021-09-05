use clap::{load_yaml, App};
use std::net::IpAddr;
use std::str::FromStr;

fn main() {
    let yaml = load_yaml!("kvs-client.yml");
    let app = App::from(yaml);
    let matches = app.get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    let addr = parse_addr(matches.value_of("addr").unwrap_or("localhost:4000"));

    let engine = matches.value_of("engine").unwrap_or("kvs");
}

fn parse_addr(addr: &str) -> IpAddr {
    IpAddr::from_str(addr).expect("bad addr format")
}
